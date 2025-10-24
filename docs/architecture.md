# RivetDB Architecture Overview

This document explains the current code layout, the responsibilities of each module, and the supporting libraries that power RivetDB.

## High-Level Architecture

RivetDB is organized into composable layers:

- **Configuration (`src/config.rs`)** — Captures node identity, listening address, peer topology, and optional storage directory. Nodes will load this from file or environment before bootstrapping the runtime.
- **Storage (`src/storage`)** — A future MVCC engine exposed through the `StorageEngine` trait. The trait already expresses the asynchronous read/stage/validate/commit flow required for optimistic concurrency control.
- **Transaction (`src/transaction`)** — Holds transaction metadata, snapshot timestamps, read-set tracking, and write intents. These structures are shared across the storage engine and upcoming Raft integration.
- **Node (`src/node.rs`)** — Glues storage and consensus layers, currently tracking node role and providing a placeholder commit path to validate trait interactions.
- **RPC (`proto/rivetdb.proto`, `src/rpc/`)** — Defines the transactional API surface for clients. The tonic build pipeline generates gRPC client/server stubs consumed by future networking work.

`src/main.rs` wires the scaffolding together by configuring tracing, instantiating a placeholder storage implementation, and constructing a `RivetNode`. A production-ready storage engine and Raft wiring will replace `NoopStorage` once those components are implemented.

## Dependency Selection

The following crates were introduced to support the architecture:

- `tokio` — Asynchronous runtime used across networking, Raft, and storage operations.
- `tonic`, `prost`, `tonic-build` — Generate and serve gRPC services for the transactional API, using Protocol Buffers for wire encoding.
- `protoc-bin-vendored` — Provides a portable `protoc` binary so builds succeed without requiring system packages.
- `serde`, `serde_json` — Serialize configuration, transaction metadata, and debugging payloads.
- `thiserror` — Derive ergonomic error types for storage and other subsystems.
- `tracing`, `tracing-subscriber` — Structured logging and diagnostics for async workflows.
- `uuid` — Generates globally unique transaction identifiers.
- `async-trait` — Enables async function signatures inside trait definitions like `StorageEngine`.
- `openraft` — Chosen Raft implementation to back replication and consensus as the system matures.
