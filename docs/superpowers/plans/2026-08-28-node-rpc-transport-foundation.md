# Node RPC Transport Foundation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a business-neutral unary Node RPC protobuf surface and a reusable per-peer tonic Channel cache to `multiraft-net` without changing existing Raft transport behavior.

**Architecture:** `multiraft-net` generates an independent `multiraft.node_rpc.NodeRpcService` whose IDs and payload remain opaque. A new `GrpcPeerChannelPool` owns the existing lazy per-Node Channel cache; `GrpcRouter` delegates only channel acquisition to the pool while retaining Raft-specific throttle, request, response, error mapping, and backoff.

**Tech Stack:** Rust 2021, Tokio, tonic 0.12, prost 0.13, vendored protoc, existing `ConnMetrics`, integration tests under `crates/multiraft-net/tests`.

**Spec:** `docs/superpowers/specs/2026-08-28-node-rpc-transport-foundation-design.md`

## Global Constraints

- Base is `origin/dev@1543c05562970c4b458310820e62088ad42a8cf4` in branch `codex/node-rpc-transport-foundation`.
- Do not modify `proto/multiraft.proto`, `grpc/server.rs`, `multiraft.rs`, FSM, Store, OpenRaft integration, demo, recovery, membership, or observation code.
- Do not add handler registry, Presence/business semantics, mTLS/authentication, batch operations, capacity/message-size policy, timeout, retry, backoff, channel eviction, reconnect policy, or a production Node RPC server owner.
- `NodeRpcRequest` is exactly `uint32 service_id`, `uint32 method_id`, and `bytes payload`; `NodeRpcResponse` is exactly `bytes payload`.
- Node RPC transport/service errors use tonic `Status`; business results remain opaque payload bytes.
- Raft and application Node RPC instantiate separate pool values with separate address catalogs.
- Catalog validation remains a caller precondition; the pool preserves current `Vec -> HashMap` behavior.
- Every new public Rust item has documentation; every fallible public method documents its exact errors.
- Every focused test command must show a nonzero intended test set. No laboratory run, ech0 pin update, push, or release is authorized.

---

### Task 1: Generate the opaque Node RPC service

**Files:**
- Create: `crates/multiraft-net/proto/node_rpc.proto`
- Create: `crates/multiraft-net/tests/node_rpc_service.rs`
- Modify: `crates/multiraft-net/build.rs`
- Modify: `crates/multiraft-net/src/grpc/mod.rs`
- Modify: `crates/multiraft-net/src/lib.rs`

**Interfaces:**
- Consumes: existing tonic/prost build pipeline and `tokio_stream::wrappers::TcpListenerStream`.
- Produces: public `multiraft_net::node_rpc` generated module containing `NodeRpcRequest`, `NodeRpcResponse`, `node_rpc_service_client::NodeRpcServiceClient`, and `node_rpc_service_server::{NodeRpcService, NodeRpcServiceServer}`.

- [ ] **Step 1: Write the failing generated-service integration test**

Create `crates/multiraft-net/tests/node_rpc_service.rs` with a real temporary tonic server. The test handler echoes the request payload for nonzero IDs and returns `Status::unimplemented` when either ID is zero. The two required tests are:

```rust
#[tokio::test]
async fn generated_node_rpc_round_trips_numeric_ids_and_opaque_payload() {
    let server = TestNodeRpcServer::start().await;
    let mut client = NodeRpcServiceClient::connect(server.endpoint()).await.unwrap();

    let response = client
        .call(NodeRpcRequest {
            service_id: 7,
            method_id: 11,
            payload: vec![0, 1, 2, 255],
        })
        .await
        .unwrap()
        .into_inner();

    assert_eq!(response.payload, vec![0, 1, 2, 255]);
    server.shutdown().await;
}

#[tokio::test]
async fn generated_node_rpc_preserves_handler_tonic_status() {
    let server = TestNodeRpcServer::start().await;
    let mut client = NodeRpcServiceClient::connect(server.endpoint()).await.unwrap();

    let status = client
        .call(NodeRpcRequest {
            service_id: 0,
            method_id: 11,
            payload: Vec::new(),
        })
        .await
        .unwrap_err();

    assert_eq!(status.code(), tonic::Code::Unimplemented);
    server.shutdown().await;
}
```

`TestNodeRpcServer` must bind `127.0.0.1:0`, serve with `serve_with_incoming_shutdown`, and retain a oneshot shutdown sender plus JoinHandle so every test explicitly stops and joins the real server.

- [ ] **Step 2: Run the focused test to verify RED**

Run:

```text
cargo test -p multiraft-net --test node_rpc_service
```

Expected: compilation fails because `multiraft_net::node_rpc` and its generated types do not exist. Record that this is the intended missing-feature failure, not a test typo.

- [ ] **Step 3: Add the exact proto and code-generation surface**

Create `proto/node_rpc.proto` with exactly:

```proto
syntax = "proto3";
package multiraft.node_rpc;

service NodeRpcService {
  rpc Call(NodeRpcRequest) returns (NodeRpcResponse);
}

message NodeRpcRequest {
  uint32 service_id = 1;
  uint32 method_id = 2;
  bytes payload = 3;
}

message NodeRpcResponse {
  bytes payload = 1;
}
```

Change `build.rs` to compile both proto files in one existing tonic-build invocation. Add a generated `node_rpc` module in `grpc/mod.rs` using `tonic::include_proto!("multiraft.node_rpc")`, and re-export that module from the crate root as `multiraft_net::node_rpc`. Do not add a registry, wrapper request, status field, or server task.

- [ ] **Step 4: Run GREEN and focused regressions**

Run:

```text
cargo test -p multiraft-net --test node_rpc_service
cargo test -p multiraft-net --test grpc_cluster
```

Expected: `node_rpc_service` runs exactly two tests and both pass; `grpc_cluster` keeps its nonzero existing test set green, proving the independent proto did not replace `RaftService`.

- [ ] **Step 5: Refactor test-only server cleanup and re-run**

Keep all startup/shutdown helpers inside `node_rpc_service.rs`; remove duplicated response construction, keep assertions on real generated client/server behavior, and rerun the two commands from Step 4.

- [ ] **Step 6: Verify and commit Task 1**

Run `cargo fmt --all -- --check`, `cargo check -p multiraft-net --all-targets`, `cargo test -p multiraft-net --test node_rpc_service`, and `git diff --check`. After fresh passing evidence, commit only Task 1 files:

```text
git commit -m "feat(net): add opaque node rpc service"
```

### Task 2: Extract the reusable peer Channel pool

**Files:**
- Create: `crates/multiraft-net/src/grpc/channel_pool.rs`
- Create: `crates/multiraft-net/tests/grpc_channel_pool.rs`
- Modify: `crates/multiraft-net/src/grpc/mod.rs`
- Modify: `crates/multiraft-net/src/lib.rs`

**Interfaces:**
- Consumes: `multiraft_core::{NodeId}`, `tonic::transport::{Channel, Endpoint}`, existing `ConnMetrics`, and Task 1's generated Node RPC client/server for real connectivity tests.
- Produces: `GrpcPeerChannelPool::{new, channel, unique_peer_links}` and `GrpcPeerChannelError::{UnknownPeer, Connect}` at the `multiraft_net` crate root.

- [ ] **Step 1: Write the failing pool integration tests**

Create `crates/multiraft-net/tests/grpc_channel_pool.rs`. Reuse a test-local real Node RPC echo server shape (do not add a production or shared test-support module). Add these observable tests:

```rust
#[tokio::test]
async fn unknown_peer_returns_the_typed_peer_identity() {
    let pool = GrpcPeerChannelPool::new(Vec::new());
    let error = pool.channel(9).await.unwrap_err();

    assert!(matches!(
        error,
        GrpcPeerChannelError::UnknownPeer { peer: 9 }
    ));
    assert_eq!(pool.unique_peer_links(), 0);
}

#[tokio::test]
async fn repeated_peer_lookup_reuses_one_cached_channel() {
    let server = TestNodeRpcServer::start(vec![1]).await;
    let pool = GrpcPeerChannelPool::new(vec![(1, server.addr())]);

    let first = pool.channel(1).await.unwrap();
    let second = pool.channel(1).await.unwrap();
    call_marker(first, 1).await;
    call_marker(second, 1).await;

    assert_eq!(pool.unique_peer_links(), 1);
    server.shutdown().await;
}

#[tokio::test]
async fn separate_pools_keep_same_node_id_address_catalogs_isolated() {
    let server_a = TestNodeRpcServer::start(vec![10]).await;
    let server_b = TestNodeRpcServer::start(vec![20]).await;
    let pool_a = GrpcPeerChannelPool::new(vec![(1, server_a.addr())]);
    let pool_b = GrpcPeerChannelPool::new(vec![(1, server_b.addr())]);

    assert_eq!(call_marker(pool_a.channel(1).await.unwrap(), 10).await, vec![10]);
    assert_eq!(call_marker(pool_b.channel(1).await.unwrap(), 20).await, vec![20]);

    server_a.shutdown().await;
    server_b.shutdown().await;
}
```

The marker server returns its configured literal byte so the isolation assertion cannot be satisfied by the wrong address.

- [ ] **Step 2: Run the focused test to verify RED**

Run:

```text
cargo test -p multiraft-net --test grpc_channel_pool
```

Expected: compilation fails because `GrpcPeerChannelPool` and `GrpcPeerChannelError` do not exist.

- [ ] **Step 3: Implement the minimal public pool and typed error**

Implement `channel_pool.rs` with the confirmed public signatures. Use `Arc<HashMap<NodeId, SocketAddr>>`, `Arc<Mutex<HashMap<NodeId, Channel>>>`, and existing `ConnMetrics`. Never hold the mutex across `.await`: check cache and drop the guard, connect, then reacquire and reuse an entry another task inserted first.

Implement `Debug`, `Display`, and `std::error::Error` manually without adding a dependency. `UnknownPeer` has no source; `Connect` exposes its tonic transport error through `source()`. Add `# Errors` documentation to `channel` and narrow documentation to `unique_peer_links` so it cannot be read as liveness.

Export the module from `grpc/mod.rs` and re-export both public types from `lib.rs`.

- [ ] **Step 4: Run GREEN and public API checks**

Run:

```text
cargo test -p multiraft-net --test grpc_channel_pool
cargo check -p multiraft-net --all-targets
```

Expected: the focused file runs exactly three tests and all pass; the complete crate public API compiles for all targets.

- [ ] **Step 5: Refactor without adding behavior**

Remove repeated endpoint/error construction inside `channel_pool.rs`, ensure error messages begin lowercase and preserve the source chain, keep the lock scopes visibly synchronous, and rerun Step 4.

- [ ] **Step 6: Verify and commit Task 2**

Run `cargo fmt --all -- --check`, `cargo test -p multiraft-net --test grpc_channel_pool`, `cargo clippy -p multiraft-net --all-targets -- -D warnings`, and `git diff --check`. After fresh passing evidence, commit only Task 2 files:

```text
git commit -m "feat(net): expose peer grpc channel pool"
```

### Task 3: Migrate the Raft router to the shared pool

**Files:**
- Modify: `crates/multiraft-net/src/grpc/router.rs`
- Test: existing `crates/multiraft-net/tests/grpc_cluster.rs`
- Test: existing `crates/multiraft-net/tests/shared_connections.rs`
- Test: existing `crates/multiraft-net/tests/chaos_failover.rs`

**Interfaces:**
- Consumes: Task 2's `GrpcPeerChannelPool` and typed error.
- Produces: unchanged public `GrpcRouter` constructors, `GroupRouter` behavior, `unique_peer_links`, Raft request wire, throttle, backoff, and OpenRaft transport-error mapping.

- [ ] **Step 1: Establish the pre-refactor characterization baseline**

Run the real existing tests before changing `router.rs`:

```text
cargo test -p multiraft-net --test grpc_cluster
cargo test -p multiraft-net --test shared_connections
```

Record the nonzero test counts and green baseline. This is the REFACTOR phase of Task 2's already-proven Channel cache behavior; do not add a source-shape test.

- [ ] **Step 2: Replace only peer-channel ownership**

Change `GrpcRouter` from separate `peers`, `channels`, and `metrics` fields to:

```rust
pub struct GrpcRouter {
    self_id: NodeId,
    channels: GrpcPeerChannelPool,
    throttle: StandbyThrottle,
}
```

`with_throttle` creates the pool from the existing peer vector. `unique_peer_links` delegates to the pool. Remove the private duplicated `channel_for`; `send` calls `self.channels.channel(to_node)` and maps either pool error into the existing `GrpcError(error.to_string()) -> Unreachable<TypeConfig>` path. Do not change request encoding, path, group, response decoding, tracing fields, throttle acquisition, or backoff.

- [ ] **Step 3: Run focused regressions after the refactor**

Run:

```text
cargo test -p multiraft-net --test grpc_channel_pool
cargo test -p multiraft-net --test grpc_cluster
cargo test -p multiraft-net --test shared_connections
```

Expected: the three new pool tests, two existing gRPC cluster tests, and one existing shared-connection test all run and pass.

- [ ] **Step 4: Run the wider network regression**

Run:

```text
cargo test -p multiraft-net
```

Expected: every non-ignored `multiraft-net` test passes, including existing failover, observation, factory, recovery, Standby containment, and snapshot cases. Do not reinterpret the local result as laboratory evidence.

- [ ] **Step 5: Verify and commit Task 3**

Run `cargo fmt --all -- --check`, `cargo check -p multiraft-net --all-targets`, `cargo clippy -p multiraft-net --all-targets -- -D warnings`, and `git diff --check`. After fresh passing evidence, commit only `router.rs`:

```text
git commit -m "refactor(net): share peer grpc channel cache"
```

### Task 4: Full integration verification and handoff

**Files:**
- Modify only if verification exposes a requirement defect: files already owned by Tasks 1-3.
- Do not modify ech0 or add evidence/laboratory artifacts.

**Interfaces:**
- Consumes: all Task 1-3 commits.
- Produces: reviewed Multi-Raft branch ready for an owner decision about push and later ech0 pin alignment.

- [ ] **Step 1: Audit scope and public contract**

Inspect `git diff 1543c05562970c4b458310820e62088ad42a8cf4...HEAD` and prove it contains only the confirmed design/plan, Node RPC proto/codegen/export, Channel pool/tests, and Raft-router refactor. Verify `multiraft.proto`, `grpc/server.rs`, and `multiraft.rs` are unchanged and search for accidental Presence, TLS, retry, timeout, batch, capacity, registry, or new server-owner code.

- [ ] **Step 2: Run full workspace gates**

Run:

```text
cargo fmt --all -- --check
cargo check --workspace --all-targets
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
git diff --check 1543c05562970c4b458310820e62088ad42a8cf4...HEAD
```

Expected: all commands exit 0; every focused/new test selected a nonzero set; ignored manual benchmarks remain reported rather than silently executed or promoted.

- [ ] **Step 3: Review and completion evidence**

Review requirement conformance first and code quality second. Confirm each new public item is documented, each pool error is matchable and source-preserving, no lock crosses `.await`, the two pool instances remain independent, Raft wire behavior is unchanged, and local tests are not described as capability-ledger evidence.

- [ ] **Step 4: Commit any verified plan-status update and stop**

If plan checkbox updates are committed, include only this plan file after all evidence is fresh. Do not push, merge, update ech0's pin, run the laboratory, release, or start Presence implementation. Report the branch commits and wait for the owner's next delivery decision.
