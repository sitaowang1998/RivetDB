# RivetDB Final Report

## Team
- Sitao Wang - 1003695101 - sitao.wang@mail.utoronto.ca

## Motivation
Operational experience with a distributed task system showed that bolting MariaDB onto a coordination plane created latency, lock contention, and scaling pain. RivetDB explores a lighter path: a transactional, Raft-replicated key-value store with MVCC so readers stay fast and writers are validated optimistically. The goal is a compact, educational prototype that demonstrates the building blocks of distributed databases without a heavyweight dependency footprint.

## Objectives
- Provide a transactional key-value API (`begin_transaction`, `get`, `put`, `commit`, `abort`) that enforces snapshot isolation.
- Replicate commits through Raft to guarantee durability and leader failover.
- Offer both in-memory and file-backed storage engines to exercise MVCC flows and persistence.
- Deliver reproducible builds, tests, and benchmarks that run on Ubuntu/macOS without manual tooling installs.

## Features
- **gRPC transactional API** (`proto/rivetdb.proto`, `src/rpc/server.rs`): begin/get/put/commit/abort plus follower-to-leader forwarding when clients talk to a follower.
- **MVCC storage engines** (`src/storage`): version chains per key with snapshot reads, OCC validation, staged writes, commit application, and abort cleanup. Both `InMemoryStorage` and JSON-backed `OnDiskStorage` are selectable at runtime via `StorageAdapter`.
- **Transaction manager** (`src/transaction.rs`): tracks snapshots, read sets, staged writes, validation, and commit timestamp allocation.
- **Raft replication** (`src/raft`): OpenRaft-backed log store, state machine that replays `ApplyTransaction` commands into the storage engine, membership tracking, leader election, snapshot scaffolding, and in-memory transport for tests.
- **Node runtime and CLI** (`src/main.rs`, `src/config.rs`): start a node with `--node-id`, `--listen-addr`, `--peer` definitions, and `--storage` backend selection. Exposes leader endpoint hints for clients.
- **Rust client library** (`src/client.rs`, `docs/client.md`): ergonomic async wrapper over the gRPC service with typed errors and transaction helpers.
- **Resilience and recovery**: disk-backed runs persist Raft state and MVCC versions; restart tests verify recovery (see `tests/recovery.rs`). Followers forward commits to the leader and block serving when in learner state.
- **Test coverage**: unit and integration suites exercise MVCC invariants, transaction conflicts, Raft leader election/failover, follower commit forwarding, client abort semantics, and restart recovery.
- **Benchmark harness** (`benchmark/`): spins up fresh 3-node clusters per run, seeds data, and records CSV results for multiple workloads; scripts trim to the middle five of seven runs.

## Performance Benchmarks
- **Environment**: Ubuntu 22.04 on dual-socket Intel Xeon E5-2630 v3 (2.40GHz, 32 vCPUs), SSD-backed storage.
- **Methodology**: Each benchmark script (see `benchmark/README.md`) spins up a fresh 3-node Raft cluster, runs the workload, tears it down, and repeats for 7 iterations. Results below use the trimmed mean (middle 5) of `duration_ms` converted to ops/s (1000 ops per run), with disk-backed storage in temp dirs by default.
- **Graphs**: Mermaid lacks a native line chart; recommended charts are line plots of throughput (ops/s) vs commit interval/read ratio/thread count using the tables below. If you prefer, I can render Mermaid flow-based visuals, but the data here is ready for a plotting tool.

### Commit frequency impact
![Commit frequency impact](benchmark/reports/graphs/commit_frequency.png)
| Workload | Commit interval (ops) | Throughput (ops/s) | Trimmed duration (ms) |
| --- | --- | --- | --- |
| Read-only | 10 | 6418.5 | 155.8 |
| Read-only | 25 | 3255.2 | 307.2 |
| Read-only | 50 | 1794.0 | 557.4 |
| Read-only | 75 | 1357.6 | 736.6 |
| Read-only | 100 | 992.5 | 1007.6 |
| Write-only | 10 | 3819.7 | 261.8 |
| Write-only | 25 | 1531.4 | 653.0 |
| Write-only | 50 | 926.8 | 1079.0 |
| Write-only | 75 | 696.8 | 1435.2 |
| Write-only | 100 | 524.5 | 1906.6 |

Takeaway: frequent commits still help throughput, especially for writes (3.8k ops/s at 10-op commits down to ~525 ops/s at 100-op commits); batching beyond ~50 ops/commit degrades throughput as Raft entries grow and validation windows widen.

### Read/write mix (100 commits, 1000 ops)
![Read/write mix](benchmark/reports/graphs/read_write_ratio.png)
| Read ratio (%) | Throughput (ops/s) | Trimmed duration (ms) |
| --- | --- | --- |
| 0 | 448.9 | 2227.6 |
| 10 | 412.6 | 2423.4 |
| 20 | 487.5 | 2051.4 |
| 30 | 547.3 | 1827.2 |
| 40 | 538.0 | 1858.6 |
| 50 | 548.8 | 1822.2 |
| 60 | 570.9 | 1751.6 |
| 70 | 601.3 | 1663.0 |
| 80 | 673.9 | 1483.8 |
| 90 | 819.7 | 1220.0 |
| 100 | 1016.9 | 983.4 |

Takeaway: throughput rises steadily as the workload skews toward reads (roughly 2.3x from 0% to 100% reads); snapshot reads stay cheap while writes pay OCC validation and Raft replication costs.

### Thread scaling (100 commits, 1000 ops)
![Thread scaling](benchmark/reports/graphs/scalability.png)
| Threads | Read throughput (ops/s) | Write throughput (ops/s) | 50/50 mixed throughput (ops/s) |
| --- | --- | --- | --- |
| 1 | 504.0 | 306.1 | 323.2 |
| 2 | 748.3 | 402.1 | 424.8 |
| 3 | 861.9 | 437.9 | 485.9 |
| 4 | 923.2 | 505.8 | 523.5 |
| 5 | 982.9 | 510.1 | 555.7 |

Takeaway: scaling is near-linear up to ~4 threads; saturation appears by 5 threads (reads ~0.98k ops/s), consistent with a single-leader commit bottleneck.

### Kill/restart resilience (leader killed mid-run)
| Workload | Scenario | Throughput (ops/s) | Trimmed duration (ms) |
| --- | --- | --- | --- | --- | --- |
| 100% reads, 100 commits | Baseline (no failure) | 992.5 | 1007.6 |
| 100% reads, 100 commits | Leader kill/restart | 746.2 | 1340.2 |
| 100% writes, 100 commits | Baseline (no failure) | 524.5 | 1906.6 |
| 100% writes, 100 commits | Leader kill/restart | 427.6 | 2338.6 |

Takeaway: the cluster survives a leader kill/restart with no failed client operations; throughput dips ~25% for reads and ~18% for writes compared to steady-state.

Raw CSV files live in `benchmark/reports/csv`; rerun with `./benchmark/scripts/*` to regenerate.

## User / Developer Guide
- **API surface**: begin -> get/put -> commit or abort. Followers forward commits to the current leader; learners reject traffic until caught up. Validation conflicts return an error string (`validation conflict`); clients should retry the transaction.
- **Client example (Rust)**:
```rust
use rivetdb::{ClientConfig, RivetClient};

# #[tokio::main]
# async fn main() -> Result<(), Box<dyn std::error::Error>> {
let client = RivetClient::connect(ClientConfig::new("http://127.0.0.1:50051")).await?;
let txn = client.begin_transaction("demo-client").await?;
assert!(txn.get("missing").await?.is_none());
txn.put("item", b"value".to_vec()).await?;
let receipt = txn.commit().await?;
println!("commit timestamp: {}", receipt.commit_ts);
# Ok(())
# }
```
- **Operational notes**:
  - Storage backends: `--storage memory` (volatile) or `--storage disk` with `--storage-path` (or `--data-dir` fallback) for persistence.
  - Raft: supply peers via `--peer <id>=<addr>` on each node; nodes start as voters unless the Raft state says otherwise. Nodes expose leader hints internally for forwarding.
  - Recovery: disk-backed runs replay committed commands from the Raft log; snapshots are stubbed but log replay restores state. Memory backend does not persist across restarts.

## Reproducibility Guide
The instructor will follow these steps verbatim on Ubuntu or macOS.

1) **Prerequisites**
   - Rust toolchain (stable). `protoc` is not required because `protoc-bin-vendored` ships a binary.
   - Open ports for gRPC (defaults in examples below).

2) **Build and test**
```bash
cargo build --release
cargo test              # integration tests bind to localhost; allow a few seconds for Raft elections
```

3) **Run a single node (memory backend)**
```bash
cargo run --release -- \
  --node-id 1 \
  --listen-addr 127.0.0.1:50051 \
  --storage memory
```

4) **Run a three-node cluster (disk backend)**
Open three terminals (adjust paths as desired):
```bash
# Terminal 1
cargo run --release -- --node-id 1 --listen-addr 127.0.0.1:6001 \
  --peer 2=127.0.0.1:6002 --peer 3=127.0.0.1:6003 \
  --storage disk --storage-path /tmp/rivet/node1 --data-dir /tmp/rivet/raft1

# Terminal 2
cargo run --release -- --node-id 2 --listen-addr 127.0.0.1:6002 \
  --peer 1=127.0.0.1:6001 --peer 3=127.0.0.1:6003 \
  --storage disk --storage-path /tmp/rivet/node2 --data-dir /tmp/rivet/raft2

# Terminal 3
cargo run --release -- --node-id 3 --listen-addr 127.0.0.1:6003 \
  --peer 1=127.0.0.1:6001 --peer 2=127.0.0.1:6002 \
  --storage disk --storage-path /tmp/rivet/node3 --data-dir /tmp/rivet/raft3
```
Then point clients at any node; followers will forward commits to the leader.

5) **Run benchmarks**
```bash
# Example: full read suite to regenerate CSVs
pushd benchmark
./scripts/read_suite.sh
./scripts/write_suite.sh
./scripts/thread_scaling_suite.sh
popd
```
Results land in `benchmark/reports/csv`; reruns overwrite existing files.

## Individual Contributions
Solo project (Sitao Wang):
- Designed and implemented MVCC storage layers (in-memory + on-disk), OCC validation, and transaction manager.
- Built gRPC service and Rust client library with transaction-aware forwarding.
- Integrated OpenRaft (log store, state machine, recovery, metrics registry) and node bootstrap/CLI.
- Authored tests for MVCC invariants, client flows, Raft elections/failover, follower forwarding, and recovery.
- Built the benchmark harness, scripts, and executed the reported experiments.

## Lessons Learned & Conclusion
- Raft serialization sets an upper bound on write throughput; batching helps but increases validation windows. Parallelism mainly benefits read-heavy workloads.
- Separating staged writes from committed versions keeps abort logic simple and makes OCC validation explicit.
- Recovery is practical via Raft log replay even with a lightweight JSON backing; snapshots would further shorten startup time.
- Remaining gaps: durable snapshot payloads, log compaction tuning, richer client retries on conflicts, and authentication for the gRPC surface.

## Video Slide Presentation
TODO - link will be added here.

## Video Demo
TODO - link will be added here.

---
Proposal content is archived in `docs/proposal.md` for reference.
