# Node RPC Transport Foundation Design

## Status and provenance

Owner-confirmed design for a narrow, business-neutral extension to
`multiraft-net`. Planning and implementation base:
`origin/dev@1543c05562970c4b458310820e62088ad42a8cf4`.

This design is source-derived. Local `cargo build` and
`cargo test --workspace` pass on the clean base, but that baseline is not new
laboratory capability evidence and does not widen any consumer claim.

## Context

`multiraft-net` already uses tonic/prost, vendored protoc, one lazy tonic
`Channel` per configured peer, and one unary Raft service carrying opaque
payload bytes. The reusable peer-channel logic is private inside `GrpcRouter`,
while the generated service and server are Raft-specific:

- `GrpcRouter::channel_for` owns peer lookup, lazy connection, a shared
  per-peer `Channel` cache, and seen-peer metrics;
- `GrpcServer` registers only `RaftServiceServer` and requires a Raft
  `GroupMap`;
- `multiraft.proto` carries `group_id`, a Raft path, and opaque payload bytes;
  and
- public `encode`/`decode` are bincode helpers whose decode path panics on
  invalid bytes.

The planned ech0 Presence work needs a separate internal gRPC instance. It
must be able to reuse Multi-Raft's generic transport mechanics without putting
Presence, User, Connection, business dispatch, or another server lifecycle
inside Multi-Raft.

## Goals

1. Extract the existing lazy, per-peer tonic Channel cache into one public,
   business-neutral type that both the Raft router and external consumers such
   as ech0 `ha` can instantiate with their own address catalogs.
2. Generate a separate unary, opaque Node RPC service whose request contains
   numeric service/method IDs and payload bytes, while leaving all business
   interpretation to the consumer.
3. Keep the existing Raft service, wire, routing, retry/backoff, throttle,
   connection-count meaning, and runtime behavior unchanged.

## Non-goals

- No Presence, UserId, Connection, Session, delivery, kick, or other IM type.
- No handler registry, business dispatch, business status, or business codec.
- No mTLS, certificate, shared-key, authentication, authorization, or signing.
- No batch operation, capacity value, project-defined message-size value,
  timeout, retry, backoff, channel eviction, or reconnect policy.
- No new production gRPC server owner, listener, task, shutdown handle, or
  readiness protocol in Multi-Raft.
- No modification to `RaftService`, `multiraft.proto`, Multi-Raft FSM, Store,
  OpenRaft integration, proposal/read behavior, or snapshot/recovery behavior.
- No ech0 dependency-pin update, laboratory run, push, or release in this
  change.

## Decision 1: public `GrpcPeerChannelPool`

Create a business-neutral channel cache with this public surface:

```rust
#[derive(Clone)]
pub struct GrpcPeerChannelPool {
    // private peer map, channel cache, and existing seen-peer metrics
}

impl GrpcPeerChannelPool {
    pub fn new(peers: Vec<(NodeId, SocketAddr)>) -> Self;

    pub async fn channel(
        &self,
        peer: NodeId,
    ) -> Result<tonic::transport::Channel, GrpcPeerChannelError>;

    pub fn unique_peer_links(&self) -> usize;
}

#[derive(Debug)]
pub enum GrpcPeerChannelError {
    UnknownPeer {
        peer: NodeId,
    },
    Connect {
        peer: NodeId,
        source: tonic::transport::Error,
    },
}
```

Semantics:

- `new` retains the current `Vec<(NodeId, SocketAddr)> -> HashMap` catalog
  behavior; catalog validation remains a caller responsibility.
- `channel` returns the cached tonic Channel when present. Otherwise it looks
  up the peer, connects to its plain internal `http://` endpoint, and inserts
  one shared Channel using the existing double-check-after-connect pattern.
- `UnknownPeer` means the requested NodeId is absent from this pool's catalog.
- `Connect` means the catalog contained the peer but tonic endpoint/channel
  construction failed. It preserves the tonic transport error as its source.
- `unique_peer_links` preserves the existing meaning: distinct peer channels
  successfully inserted into this cache. It is not current connectivity,
  liveness, membership, readiness, or quorum state.

Refactor `GrpcRouter` to retain `self_id` and `StandbyThrottle`, but replace its
peer map, channel map, and metrics fields with one `GrpcPeerChannelPool`.
`GrpcRouter::send` obtains a Channel from the pool, maps the generic pool error
to the existing OpenRaft `Unreachable` classification, and otherwise preserves
the current Raft request, response, logging, throttle, and backoff behavior.

Raft and Node RPC do not share a pool instance:

```text
Raft GrpcRouter -> GrpcPeerChannelPool(Raft peer addresses)
ech0 ha Node RPC -> GrpcPeerChannelPool(Node RPC addresses)
```

They share only the type and implementation.

## Decision 2: separate opaque `NodeRpcService`

Add `proto/node_rpc.proto` as an independent protobuf package:

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

The existing build script compiles both `multiraft.proto` and
`node_rpc.proto`. `multiraft-net` exports the generated Node RPC module so a
consumer can construct `NodeRpcServiceClient`, implement the generated
`NodeRpcService` trait, and construct `NodeRpcServiceServer`.

Multi-Raft does not interpret or allocate service/method IDs, inspect payload
bytes, register handlers, map business errors, or start a Node RPC listener.
Transport/service failures use tonic `Status`; a successful call returns only
opaque response payload bytes. The consumer owns unknown-service and
unknown-method behavior and every business result encoding.

The new service is not added to the existing `GrpcServer`. An ech0 consumer
will create and own a separate tonic server, listener, task, and shutdown path.

## Public API and file ownership

Create:

- `crates/multiraft-net/src/grpc/channel_pool.rs`: public pool and typed error.
- `crates/multiraft-net/proto/node_rpc.proto`: opaque Node RPC service.
- `crates/multiraft-net/tests/grpc_channel_pool.rs`: pool behavior and Raft
  integration regression.
- `crates/multiraft-net/tests/node_rpc_service.rs`: generated service/client
  roundtrip and tonic Status propagation.

Modify:

- `crates/multiraft-net/build.rs`: compile both proto inputs.
- `crates/multiraft-net/src/grpc/mod.rs`: include/export the generated Node RPC
  module and the channel-pool module.
- `crates/multiraft-net/src/grpc/router.rs`: delegate peer-channel ownership to
  `GrpcPeerChannelPool` and preserve Raft error mapping.
- `crates/multiraft-net/src/lib.rs`: re-export the pool, error, and generated
  Node RPC module.

Do not modify:

- `crates/multiraft-net/proto/multiraft.proto`;
- `crates/multiraft-net/src/grpc/server.rs`;
- `crates/multiraft-net/src/multiraft.rs`; or
- any FSM, Store, OpenRaft, demo, recovery, membership, observation, or
  application repository file.

## Error and lifecycle boundaries

The pool reports only catalog absence or tonic connection failure. It does not
retry, wait on a caller-defined deadline, or translate errors into OpenRaft or
business terms. `GrpcRouter` alone retains its existing OpenRaft mapping.

The generated Node RPC service provides no runtime ownership. Tests may start a
temporary tonic server to prove generated client/server interoperability, but
no new production task or server handle is added to Multi-Raft. The existing
incomplete `MultiRaft` gRPC shutdown contract is neither copied nor changed.

## Testing

Use RED-GREEN-REFACTOR for each behavior.

Focused pool tests prove:

- unknown peer returns `GrpcPeerChannelError::UnknownPeer`;
- a configured live peer produces a tonic Channel;
- repeated lookup reuses the cache and leaves `unique_peer_links() == 1`;
- two pool instances can map the same NodeId to different addresses without
  sharing connections; and
- the refactored Raft router retains the existing O(nodes), not O(groups),
  connection behavior.

Focused Node RPC tests prove:

- generated request/response fields preserve `u32`, `u32`, and opaque bytes;
- a temporary echo implementation round-trips service ID, method ID, and
  payload through generated client/server types;
- a handler tonic `Status` reaches the client as a transport/service error;
  and
- the existing Raft protobuf/service remains independently callable.

Regression gates:

```text
cargo test -p multiraft-net --test grpc_channel_pool
cargo test -p multiraft-net --test node_rpc_service
cargo test -p multiraft-net --test grpc_cluster
cargo test -p multiraft-net --test shared_connections
cargo fmt --all -- --check
cargo check --workspace --all-targets
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
```

Every filtered test command must report a nonzero intended test count.

## Risks and containment

- Exposing tonic `Channel` intentionally couples this transport utility's
  public API to the already selected tonic version. The extension is explicitly
  a tonic transport foundation, so this is accepted rather than hidden behind
  another abstraction.
- The pool does not validate duplicate NodeIds or address policy. Consumers
  must pass a validated catalog; this preserves current behavior and avoids
  inventing deployment policy here.
- Tonic retains its library defaults, including its own message handling
  behavior. This change adds no project capacity claim and does not treat a
  dependency default as a product limit derived from load evidence.
- The existing metric records peers whose channels were inserted; it does not
  decrement. The API name and documentation must preserve that narrow meaning.

## Delivery boundary

This Multi-Raft change ends after source, focused/regression tests, workspace
gates, and review. It does not update ech0's pin, Presence proposal, or consumer
code. Commit, push, consumer alignment, laboratory execution, and release are
separate owner decisions.
