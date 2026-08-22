# Architecture notes

**中文：** [ARCHITECTURE.zh-CN.md](./ARCHITECTURE.zh-CN.md)

Thin Multi-Raft runtime for matching HA. One Raft group per trading symbol
(`GroupId`); shared peer connections; pluggable FSM. Built on
`openraft` + `openraft-multi` (pinned).

## Phase rule

1. **Phase-1 (this repo):** library + multi-process demo + chaos / Jepsen.
   No RocketMQ, no matching engine FSM.
2. **Phase-2 (downstream app):** optional Leader RMQ consume → `propose`;
   pluggable matching engine FSM. Followers do not consume ingress.
3. Do not pull match DTOs / RMQ into `multiraft-*` crates.

## Crate responsibilities

```text
crates/
├── multiraft-core/   # TypeConfig, ClusterConfig, MultiRaftError, ProposeOk, ObservationClosed
├── multiraft-net/    # Shared GroupRouter / GrpcRouter + MultiRaft facade + normalized observation
├── multiraft-fsm/    # StateMachine trait (apply / snapshot / restore)
├── multiraft-store/  # Per-group file-backed log / state / snapshot
└── multiraft-demo/   # 3-node × N-group CounterFsm + admin HTTP
```

| Crate | Does | Does not |
|-------|------|----------|
| `multiraft-core` | Shared types / errors, including typed observation closure | Networking, storage |
| `multiraft-net` | `MultiRaft` API, O(nodes) links, generic FSM factory injection, backend-neutral group observation | Business type selection, command semantics, durable business metadata, business registry, discriminator, or HA policy |
| `multiraft-fsm` | Trait + demo `CounterFsm` | Depend on a matching engine FSM |
| `multiraft-store` | Per-group persistence | Order book |
| `multiraft-demo` | Acceptance / Jepsen target | Production deploy |

## Topology

```text
                    ┌──────────────────────────────────────┐
  Admin HTTP        │  OS process = one Raft node          │
  (per node)        │  MultiRaft + N groups (shared gRPC)  │
                    └───────────────┬──────────────────────┘
                                    │ tonic / openraft-multi
                    ┌───────────────┼──────────────────────┐
                    ▼               ▼                      ▼
                 node-1          node-2                 node-3
              groups 0..N-1   groups 0..N-1          groups 0..N-1
```

- `--mode node`: one OS process per Raft node (production-shaped).
- `--mode cluster`: in-process 3-logical-node harness for fast tests.
- Peer links: **O(nodes)**, not O(groups). `unique_peer_links()` exposes this.

## Data flow (phase-2 target)

```text
RMQ (per-symbol)
  → [Leader only] validate → propose(group, cmd)
  → openraft quorum commit → FSM.apply on all replicas
  → [Leader] egress / ack RMQ after commit+apply
```

Phase-1 demo injects `propose` locally (`POST /groups/{id}/inc` or background loop).

## Consistency Contract (per group)

| API | Model |
|-----|--------|
| `propose` → Ok | Linearizable write (committed + applied) |
| `read_linearizable` | Linearizable read (ReadIndex) |
| `read_stale` | Local + applied watermark; requires `enable_stale_queries` (Standby offload) |
| `with_fsm` | Local / may be stale — debug / metrics |
| `observe_group` | Local control observation only; latest/coalescing |
| Cross-group | No cross-symbol transactions |

Failed / timed-out `propose` is **indeterminate** — retry with the same idempotency key.

Details: [specs/2026-07-18-multiraft-design.md](./specs/2026-07-18-multiraft-design.md) · [中文](./specs/2026-07-18-multiraft-design.zh-CN.md) §4.3.1,
[jepsen.md](./jepsen.md) · [中文](./jepsen.zh-CN.md).

## Normalized group observation

`MultiRaft::observe_group(group)` exposes an initial `GroupObservation` and a
single-owner `GroupObservationReceiver`. It is a stateless adapter over one
OpenRaft `server_metrics()` receiver for one local Raft instance. It does not
read full `metrics()`, `data_metrics()`, FSM data, snapshot catalogs, Standby
restore state, or transport diagnostics, and it does not create a background
task, cache, fan-out layer, timer, persistence, or second HA owner.

The normalized value contains:

- `group_id`, `local_node_id`, `local_membership_role`;
- `server_state`, `leader_hint`, `flushed_vote`;
- `effective_membership` and `committed_membership`.

`local_membership_role` is derived only from effective membership, never from
static `ClusterConfig::role`. Each membership observation keeps joint voter
configs as `Vec<BTreeSet<NodeId>>`, keeps learners separate, and preserves the
complete membership log identity as `{term, node_id, index}`. Node addresses
remain peer-catalog configuration, not HA facts, and are not exposed.

The receiver has latest-value watch semantics: intermediate states may be
coalesced, there is no history or replay, and there is no `current()`.
`ObservationClosed` is the typed terminal result. If the local Raft instance is
shut down and a same-id group is later restarted, the old receiver remains
closed; consumers must call `observe_group()` again on the new `MultiRaft`
instance.

Leader hints and membership role are observations, not capability claims. They
do not prove writable, readable, available, healthy, quorum, lease, epoch, or
generation state. Actual write permission is revalidated by `propose`; actual
linearizable read permission is revalidated by ReadIndex. `on_leader_change()`
remains the older best-effort compatibility callback and is not rewritten by
this observer.

## Downstream integration (phase 2)

```text
matching process / ingress shell (RMQ consumer, Leader only)
  → multiraft::MultiRaft (propose / leader callbacks)
    → FSM adapter → matching engine FSM
```

### Generic FSM factory and lifecycle boundary

`multiraft-net` owns the generic injection mechanics, not business type
selection, business command semantics, durable business metadata, a business
registry, or a discriminator. Applications provide `StateMachineFactory<S>`;
each successful result from a factory invoked with `FsmFactoryContext` is a
separately owned `S` for one local group. `CounterFsm` remains the demo/default
path.

The factory is synchronous, lightweight, and non-blocking. It must not perform
network work, start irreversible side effects, or start background tasks.
Factory construction is not exactly-once: calls may repeat after a factory or
other pre-publication failure and after process restart, and calls for different
`(node_id, group_id)` keys may be concurrent. Until separate lifecycle work
lands, callers must serialize same-key `create_group` calls.

There is no factory rollback callback. A factory error, or a pre-insertion
FileLog/Raft construction failure after the factory returns, leaves the group
unpublished. Drop-counter coverage proves prompt release of the returned FSM
only for the tested default `NodeRole::Voter` with `SnapshotMode::Disabled`
FileLog-open failure path. It explicitly excludes `StandbyOffload`: the current
state-machine-store/trigger/holder strong-reference cycle can retain the FSM,
so prompt release is not guaranteed. Factories must avoid irreversible side
effects and must not rely on prompt drop outside that proven path. After registry
insertion, `try_initialize` can still return an error after the group has been
published. This boundary does not define snapshot restore, business-store
coordination, or any group-lifecycle fix.

## Standby async snapshot

`SnapshotMode::StandbyOffload` is contained for v1. `STANDBY=1` remains a lab
learner/catalog/checksum/advertisement-generation workflow, but the catalog is
not a current snapshot provider and live HTTP/ad/catalog/daisy restoration is
typed unsupported. Normal OpenRaft recovery remains authoritative. The Factory
6677 design intentionally routed Catalog -> `current_snapshot` -> OpenRaft and
ad/HTTP -> direct install; this candidate withdraws that first contract because
C42 and the available metadata do not establish complete restore ownership.

The contained historical flow was precheck -> tail apply -> FSM-only restore ->
divergence. Any future restoration requires a separately Accepted complete
envelope, atomic capture, Vote/full `LogId`/membership, and
`install_full_snapshot`; it is not shipped or scheduled by this repository.

Details: [specs/2026-07-20-standby-async-snapshot-design.md](./specs/2026-07-20-standby-async-snapshot-design.md)
· [中文](./specs/2026-07-20-standby-async-snapshot-design.zh-CN.md).

Learner membership, throttling, promote/demote, and `read_stale` remain separate
from live restore. Historical HTTP/ad/catalog/daisy restoration claims are
contained:
[specs/2026-07-20-aeron-standby-parity-design.md](./specs/2026-07-20-aeron-standby-parity-design.md)
· [中文](./specs/2026-07-20-aeron-standby-parity-design.zh-CN.md).

P2 daisy is a **snapshot distribution chain** (not openraft log redirect).
P3 `read_stale` is explicitly non-linearizable.

## Upstream pin

| Crate | Version |
|-------|---------|
| `openraft` | `=0.10.0-alpha.30` |
| `openraft-multi` | `=0.10.0-alpha.30` |

See [upstream.md](./upstream.md) · [中文](./upstream.zh-CN.md).
