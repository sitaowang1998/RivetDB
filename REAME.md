# RivetDB: Distributed Versioned Key-Value Store with MVCC and Raft Replication

## Motivation

This project arises from experience building a [distributed task execution system] where nodes
needed a transactional database for coordinating task states. The system initially relied on 
`MariaDB`, but performance quickly became problematic. MariaDB introduced latency and contention
under distributed workloads. To address these limitations, this project proposes **RivetDB**, a
lightweight, transactional, distributed key-value database that combines **Multi-Version Concurrency
Control (MVCC)** with **Raft replication**. The system maintains strong consistency and fault 
tolerance while remaining compact.

## Objectives and Key Features

### Objective
Build a minimal, correct, and fault-tolerant distributed key-value store with transactional
semantics. The system will demonstrate the core mechanisms behind distributed databases—versioning,
optimistic concurrency, replication, and recovery.

### Key features

* **Transactional key-value API** with `GET`, `PUT`, `BEGIN_TRANSACTION`, `COMMIT`, and `ABORT`.
* **MVCC (Multi-Version Concurrency Control)** for snapshot isolation and non-blocking reads.
* **Optimistic Concurrency Control (OCC)** for validating transactions at commit time.
* **Raft replication** to ensure strong consistency and durability across replicas.
* **Crash safety:** Only committed transactions are replicated; uncommitted changes are never
  applied.

## System Architecture

The system has three major components:

1. **Client Library:** A Rust client that manages transaction contexts, assigns transaction IDs, and
   sends operations to the cluster leader.
2. **Storage Node:** Each node stores versioned key-value data and maintains a Raft state machine.
   The Raft leader appends commit records and coordinates replication.
3. **Coordinator Role:** The leader of the Raft group acts as the coordinator, validating and
   committing transactions.

All data changes pass through Raft replication, guaranteeing atomic and consistent updates even in
the presence of node crashes. Each node can reconstruct its state entirely from the Raft log,
ensuring durability without needing a separate write-ahead log.

## Concurrency Control and Transactions

RivetDB uses MVCC combined with optimistic concurrency control (OCC) to manage concurrent
transactions.

* Each key maintains multiple versions, identified by commit timestamps.
* Transactions read from a consistent snapshot determined at `BEGIN_TRANSACTION`.
* During execution, reads and writes are recorded in a transaction context but not applied to
  storage.
* On `COMMIT`, the leader validates that no newer committed versions exist for any keys in the read
  set.
* If validation succeeds, a `COMMIT` entry containing all writes is appended to the Raft log.
* The transaction becomes visible when Raft replication reaches quorum and the log entry is
  committed.

If validation fails or the client aborts, the transaction is discarded. Because only committed
transactions are replicated, the system never exposes partial or inconsistent updates.

### Snapshot Reads

Using MVCC, readers never block writers. Each read retrieves the latest committed version less than
or equal to the transaction’s snapshot timestamp. This ensures repeatable reads and prevents
anomalies like dirty or non-repeatable reads.

### Commit Atomicity

Raft ensures that a commit entry is applied consistently across all replicas. Once a majority
acknowledges the entry, it is guaranteed to persist. If the leader crashes mid-commit, the new
leader continues replication from the existing Raft log, ensuring atomicity.

## Crash Handling

RivetDB prioritizes safety over availability during crashes. It ensures that:

* **Client crash before commit:** No Raft entry is created, so no changes reach the database. The
  system remains consistent.
* **Node crash:** The node recovers by replaying its Raft log. Any unapplied but committed entries
  are applied upon recovery.
* **Leader crash:** Raft elects a new leader. Because commit decisions are replicated, the new
  leader continues safely.

No special client-side recovery logic is required; the system treats any incomplete transaction as
aborted. This greatly simplifies failure handling compared to traditional 2PC-based systems.

## Tentative Schedule

The project spans **10 weeks**, structured to balance design, implementation, and validation within
realistic single-person scope.

* **Week 1 (Oct 13 – Oct 19)** — *Design & Setup*

  Define architecture (MVCC + Raft), choose libraries, and design RPC interfaces.

* **Week 2 (Oct 20 – Oct 26)** — *Core Storage Engine*

  Implement in-memory key-value store with version chains per key. Support snapshot reads.

* **Week 3 (Oct 27 – Nov 2)** — *Transaction Manager*

  Implement transaction context, OCC-based validation, and commit logic.

* **Week 4 (Nov 3 – Nov 9)** — *Networking & RPC*

  Add gRPC-based API for transactions and build a simple CLI client.

* **Week 5 (Nov 10 – Nov 16)** — *Raft Integration*

  Integrate an existing Raft library (e.g., [`openraft`]), enabling replication and leader election.

* **Week 6 (Nov 17 – Nov 23)** — *Combine MVCC + Raft*

  Ensure commits are replicated and applied only after Raft consensus.

* **Week 7 (Nov 24 – Nov 30)** — *Crash Recovery*

  Implement log replay, node recovery, and safe transaction cleanup.

* **Week 8 (Dec 1 – Dec 7)** — *Testing & Validation*

  Test concurrency, isolation, and crash recovery scenarios.

* **Week 9 (Dec 8 – Dec 14)** — *Optimization*

  Reduce commit latency, implement caching for read-mostly workloads.

* **Week 10 (Dec 15 – Dec 21)** — *Finalization*

  Complete documentation, prepare demo and final report.

## Expected Outcomes

By the project’s completion, RivetDB will:

* Provide a working prototype demonstrating distributed transactional semantics.
* Support consistent reads and writes under MVCC.
* Achieve fault tolerance and crash recovery using Raft replication alone.

[distributed task execution system]: https://github.com/y-scope/spider
[`openraft`]: https://github.com/databendlabs/openraft
